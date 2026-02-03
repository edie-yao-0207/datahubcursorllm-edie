package databricks_test

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"samsaradev.io/helpers/samtime"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/libs/ni/pointer"
)

func TestExecuteStatementOutputUnmarshal(t *testing.T) {
	jsonToDecode := `{
	"statement_id": "01eda0ea-9b4b-15ce-b8bb-a7d4114cb5ed",
	"status": {
		"state": "SUCCEEDED"
	},
	"manifest": {
		"format": "CSV",
		"schema": {
		"column_count": 1,
		"columns": [
			{
			"name": "id",
			"position": 0,
			"type_name": "LONG",
			"type_text": "BIGINT"
			}
		]
		},
		"total_chunk_count": 1,
		"chunks": [
		{
			"chunk_index": 0,
			"row_offset": 0,
			"row_count": 100
		}
		],
		"total_row_count": 100,
		"total_byte_count": 293
	},
	"result": {
		"external_links": [
		{
			"chunk_index": 0,
			"row_offset": 0,
			"row_count": 100,
			"byte_count": 293,
			"external_link": "https://someplace.cloud-provider.com/very/long/path/...",
			"expiration": "2023-01-30T22:23:23.14Z"
		}
		]
	}
	}`

	var output databricks.ExecuteStatementOutput
	err := json.Unmarshal([]byte(jsonToDecode), &output)
	require.NoError(t, err)

	marshalled, err := json.Marshal(output)
	require.NoError(t, err)

	compacted, err := CompactJSON(jsonToDecode)
	require.NoError(t, err)

	require.Equal(t, compacted, string(marshalled))
}

type testStruct struct {
	ID          int       `json:"id"`
	Name        string    `json:"name"`
	Active      bool      `json:"active"`
	CreatedAt   time.Time `json:"created_at"`
	IntVal      int       `json:"int_val"`
	LongVal     int64     `json:"long_val"`
	FloatVal    float32   `json:"float_val"`
	DoubleVal   float64   `json:"double_val"`
	DecimalVal  float64   `json:"decimal_val"`
	NullableVal *string   `json:"nullable_val"`
	WithNoTag   string    `json:"-"`
	StringArray []string  `json:"string_array"`
	IntArray    []int     `json:"int_array"`
}

func TestScanRows(t *testing.T) {
	tests := []struct {
		name        string
		output      *databricks.ExecuteStatementOutput
		want        []testStruct
		shouldError bool
	}{
		{
			name: "basic types",
			output: &databricks.ExecuteStatementOutput{
				Manifest: &databricks.Manifest{
					Schema: databricks.Schema{
						Columns: []databricks.Column{
							{Name: "id", TypeName: databricks.TypeNameInt},
							{Name: "name", TypeName: databricks.TypeNameString},
							{Name: "active", TypeName: databricks.TypeNameBoolean},
							{Name: "created_at", TypeName: databricks.TypeNameTimestamp},
						},
					},
				},
				Result: &databricks.Result{
					DataArray: [][]*string{
						{
							pointer.New("1"),
							pointer.New("John Doe"),
							pointer.New("true"),
							pointer.New(samtime.TestRefTime.Format(time.RFC3339)),
						},
						{
							pointer.New("2"),
							pointer.New("Jane Smith"),
							pointer.New("false"),
							pointer.New(samtime.TestRefTime.Format(time.RFC3339)),
						},
					},
				},
			},
			want: []testStruct{
				{
					ID:        1,
					Name:      "John Doe",
					Active:    true,
					CreatedAt: samtime.TestRefTime,
				},
				{
					ID:        2,
					Name:      "Jane Smith",
					Active:    false,
					CreatedAt: samtime.TestRefTime,
				},
			},
		},
		{
			name: "null values",
			output: &databricks.ExecuteStatementOutput{
				Manifest: &databricks.Manifest{
					Schema: databricks.Schema{
						Columns: []databricks.Column{
							{Name: "nullable_val", TypeName: databricks.TypeNameString},
						},
					},
				},
				Result: &databricks.Result{
					DataArray: [][]*string{
						{nil},
						{pointer.New("Jane")},
					},
				},
			},
			want: []testStruct{
				{NullableVal: nil},
				{NullableVal: pointer.New("Jane")},
			},
		},
		{
			name: "numeric types",
			output: &databricks.ExecuteStatementOutput{
				Manifest: &databricks.Manifest{
					Schema: databricks.Schema{
						Columns: []databricks.Column{
							{Name: "int_val", TypeName: databricks.TypeNameInt},
							{Name: "long_val", TypeName: databricks.TypeNameLong},
							{Name: "float_val", TypeName: databricks.TypeNameFloat},
							{Name: "double_val", TypeName: databricks.TypeNameDouble},
							{Name: "decimal_val", TypeName: databricks.TypeNameDecimal},
						},
					},
				},
				Result: &databricks.Result{
					DataArray: [][]*string{
						{
							pointer.New("123"),
							pointer.New("456"),
							pointer.New("1.23"),
							pointer.New("4.56"),
							pointer.New("123.456"),
						},
					},
				},
			},
			want: []testStruct{
				{
					IntVal:     123,
					LongVal:    456,
					FloatVal:   1.23,
					DoubleVal:  4.56,
					DecimalVal: 123.456,
				},
			},
		},
		{
			name: "array types",
			output: &databricks.ExecuteStatementOutput{
				Manifest: &databricks.Manifest{
					Schema: databricks.Schema{
						Columns: []databricks.Column{
							{Name: "string_array", TypeName: databricks.TypeNameArray},
							{Name: "int_array", TypeName: databricks.TypeNameArray},
						},
					},
				},
				Result: &databricks.Result{
					DataArray: [][]*string{
						{
							pointer.New("[\"Item 1\",\"Item 2\",\"Item 3\"]"),
							pointer.New("[1,2,3]"),
						},
					},
				},
			},
			want: []testStruct{
				{
					StringArray: []string{"Item 1", "Item 2", "Item 3"},
					IntArray:    []int{1, 2, 3},
				},
			},
		},
		{
			name: "field with no tag",
			output: &databricks.ExecuteStatementOutput{
				Manifest: &databricks.Manifest{
					Schema: databricks.Schema{
						Columns: []databricks.Column{
							{Name: "with_no_tag", TypeName: databricks.TypeNameString},
						},
					},
				},
				Result: &databricks.Result{
					DataArray: [][]*string{
						{
							pointer.New("no tag field"),
						},
					},
				},
			},
			want: []testStruct{
				{
					WithNoTag: "no tag field",
				},
			},
		},
		{
			name: "should return error when no result in response",
			output: &databricks.ExecuteStatementOutput{
				Result: nil,
			},
			shouldError: true,
		},
		{
			name: "should return error when no schema in response",
			output: &databricks.ExecuteStatementOutput{
				Result:   &databricks.Result{},
				Manifest: nil,
			},
			shouldError: true,
		},
		{
			name: "should return error when type conversion fails",
			output: &databricks.ExecuteStatementOutput{
				Manifest: &databricks.Manifest{
					Schema: databricks.Schema{
						Columns: []databricks.Column{
							{Name: "id", TypeName: databricks.TypeNameInt},
						},
					},
				},
				Result: &databricks.Result{
					DataArray: [][]*string{
						{pointer.New("not a number")},
					},
				},
			},
			shouldError: true,
		},
		{
			name: "should return error for invalid row length",
			output: &databricks.ExecuteStatementOutput{
				Manifest: &databricks.Manifest{
					Schema: databricks.Schema{
						Columns: []databricks.Column{
							{Name: "id", TypeName: databricks.TypeNameInt},
						},
					},
				},
				Result: &databricks.Result{
					DataArray: [][]*string{
						{
							pointer.New("123"),
							pointer.New("456"),
							pointer.New("1.23"),
							pointer.New("4.56"),
						},
					},
				},
			},
			shouldError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create an empty instance of the target type
			var result []testStruct
			err := tt.output.ScanRows(&result)
			if tt.shouldError {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.want, result)
		})
	}
}

func CompactJSON(jsonStr string) (string, error) {
	// First ensure it's valid JSON by parsing it
	var temp interface{}
	if err := json.Unmarshal([]byte(jsonStr), &temp); err != nil {
		return "", err
	}

	// Create a buffer to store the compacted JSON
	var buf bytes.Buffer

	// Compact the JSON, removing all whitespace
	if err := json.Compact(&buf, []byte(jsonStr)); err != nil {
		return "", err
	}

	return buf.String(), nil
}
