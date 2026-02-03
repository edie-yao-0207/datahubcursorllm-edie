package main

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"samsaradev.io/helpers/ni/filepathhelpers"
	"samsaradev.io/infra/dataplatform/rdsdeltalake"
	"samsaradev.io/infra/dataplatform/sqlschemaparser"
)

func TestParseDBSchemaFile(t *testing.T) {
	testDbSchemaPath := filepath.Join(filepathhelpers.BackendRoot, "go/src/samsaradev.io/infra/dataplatform/rdsdeltalake/generate/testdata/test_db_schema.sql")
	db, err := sqlschemaparser.ParseDBSchemaFile(testDbSchemaPath)
	require.NoError(t, err)

	// test_table_1 schema
	require.Equal(t, sqlschemaparser.Table{
		Columns: []sqlschemaparser.Column{
			{
				ColName: "test_col_1",
				ColType: "bigint(20)",
			},
			{
				ColName: "test_col_2",
				ColType: "smallint(6)",
			},
			{
				ColName: "test_col_3",
				ColType: "blob",
			},
			{
				ColName: "test_col_4",
				ColType: "tinyint(1)",
			},
		},
	}, *db.Tables["test_table_1"])

	// test_table_2 schema
	require.Equal(t, sqlschemaparser.Table{
		Columns: []sqlschemaparser.Column{
			{
				ColName: "test_col_1",
				ColType: "bigint(20)",
			},
			{
				ColName: "test_col_2",
				ColType: "varchar(2083)",
			},
			{
				ColName: "test_col_3",
				ColType: "blob",
			},
		},
	}, *db.Tables["test_table_2"])

	// test_table_3 schema
	require.Equal(t, sqlschemaparser.Table{
		Columns: []sqlschemaparser.Column{
			{
				ColName: "test_col_1",
				ColType: "bigint(20)",
			},
			{
				ColName: "test_col_2",
				ColType: "bigint(20)",
			},
		},
	}, *db.Tables["test_table_3"])

	// test_table_4 schema
	require.Equal(t, sqlschemaparser.Table{
		Columns: []sqlschemaparser.Column{
			{
				ColName: "test_col_1",
				ColType: "bigint(20)",
			},
			{
				ColName: "test_col_2",
				ColType: "bigint(20)",
			},
			{
				ColName: "test_col_3",
				ColType: "datetime(6)",
			},
		},
	}, *db.Tables["test_table_4"])
}

func TestIsColumnExcludedFromReplication(t *testing.T) {
	testCases := []struct {
		description      string
		columnName       string
		columnsToExclude []string
		expected         bool
	}{
		{
			description:      "should return false if columnsToExclude is empty",
			columnName:       "test_column",
			columnsToExclude: []string{},
			expected:         false,
		},
		{
			description:      "should return false if column name is not in columnsToExclude",
			columnName:       "test_column",
			columnsToExclude: []string{"test_column_2"},
			expected:         false,
		},
		{
			description:      "should return true if column name is in columnsToExclude",
			columnName:       "test_column",
			columnsToExclude: []string{"test_column"},
			expected:         true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			actual := isColumnExcludedFromReplication(tc.columnName, tc.columnsToExclude)
			assert.Equal(t, tc.expected, actual)
		})
	}
}

func TestValidatePartitionStrategy(t *testing.T) {
	makeTable := func(cols map[string]string) *sqlschemaparser.Table {
		var columns []sqlschemaparser.Column
		for name, typ := range cols {
			columns = append(columns, sqlschemaparser.Column{ColName: name, ColType: typ})
		}
		return &sqlschemaparser.Table{Columns: columns}
	}

	// Success cases
	t.Run("milliseconds ok on bigint", func(t *testing.T) {
		tbl := makeTable(map[string]string{
			"ms": "bigint(20)",
		})
		require.NoError(t, validatePartitionStrategy("tbl", tbl, rdsdeltalake.DateColumnFromMilliseconds("ms")))
	})

	t.Run("timestamp ok on datetime", func(t *testing.T) {
		tbl := makeTable(map[string]string{
			"ts": "datetime(6)",
		})
		require.NoError(t, validatePartitionStrategy("tbl", tbl, rdsdeltalake.DateColumnFromTimestamp("ts")))
	})

	t.Run("date ok on date", func(t *testing.T) {
		tbl := makeTable(map[string]string{
			"dt": "date",
		})
		ps := rdsdeltalake.PartitionStrategy{Key: "date", SourceColumnType: rdsdeltalake.DatePartitionColumnType, SourceColumnName: "dt"}
		require.NoError(t, validatePartitionStrategy("tbl", tbl, ps))
	})

	// Error cases
	t.Run("column not found", func(t *testing.T) {
		tbl := makeTable(map[string]string{
			"present": "bigint(20)",
		})
		err := validatePartitionStrategy("tbl", tbl, rdsdeltalake.DateColumnFromMilliseconds("missing"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "does not exist in table schema")
	})

	t.Run("milliseconds wrong type", func(t *testing.T) {
		tbl := makeTable(map[string]string{
			"ms": "varchar(10)",
		})
		err := validatePartitionStrategy("tbl", tbl, rdsdeltalake.DateColumnFromMilliseconds("ms"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "DateColumnFromMilliseconds()")
	})

	t.Run("timestamp wrong type", func(t *testing.T) {
		tbl := makeTable(map[string]string{
			"ts": "bigint(20)",
		})
		err := validatePartitionStrategy("tbl", tbl, rdsdeltalake.DateColumnFromTimestamp("ts"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "DateColumnFromTimestamp()")
	})

	t.Run("date wrong type", func(t *testing.T) {
		tbl := makeTable(map[string]string{
			"dt": "varchar(32)",
		})
		ps := rdsdeltalake.PartitionStrategy{Key: "date", SourceColumnType: rdsdeltalake.DatePartitionColumnType, SourceColumnName: "dt"}
		err := validatePartitionStrategy("tbl", tbl, ps)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "should be a date MySQL column")
	})

	t.Run("invalid partition source type", func(t *testing.T) {
		tbl := makeTable(map[string]string{
			"ms": "bigint(20)",
		})
		ps := rdsdeltalake.PartitionStrategy{Key: "date", SourceColumnType: rdsdeltalake.PartitionColumnType("invalid"), SourceColumnName: "ms"}
		err := validatePartitionStrategy("tbl", tbl, ps)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Invalid partition strategy source column type")
	})
}
