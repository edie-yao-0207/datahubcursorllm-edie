package outputtypes

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseRawOutput(t *testing.T) {
	output := &TableOutput{
		BaseOutput: BaseOutput{
			Type: TableOutputType,
		},
		DBName:     "db1",
		TableName:  "t1",
		Partition:  []string{"date"},
		PrimaryKey: []string{"date", "org_id", "end_ms"},
		Schema: &Schema{
			{
				Name: "date",
				Type: "date",
			},
			{
				Name: "org_id",
				Type: "bigint",
			},
			{
				Name: "end_ms",
				Type: "datetime",
			},
			{
				Name:     "nullable_column",
				Type:     "string",
				Nullable: true,
			},
			{
				Name: "nested_column",
				Type: "struct",
				NestedFields: []Column{
					{
						Name: "col1",
						Type: "bigint",
					},
					{
						Name: "col2",
						Type: "string",
					},
				},
			},
		},
	}

	rawOutput, err := json.Marshal(output)
	require.NoError(t, err)
	parsedOutput, err := ParseRawOutput(rawOutput)
	require.NoError(t, err)
	require.Equal(t, output, parsedOutput)
}
