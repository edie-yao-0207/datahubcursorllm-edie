package databricks

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"samsaradev.io/infra/dataplatform/databricks"
)

func TestFlattenExpandQuery(t *testing.T) {
	query := &databricks.Query{
		DataSourceID: "abc123",
		Name:         "name",
		Description:  "description",
		Query:        "SELECT 1+1 AS 2",
		Tags:         []string{"tag1", "tag2"},
		Parent:       "/backend",
		RunAsRole:    "viewer",
		Options: &databricks.QueryOptions{
			Parameters: []interface{}{
				&databricks.QueryParameterText{
					QueryParameter: databricks.QueryParameter{
						Name:  "text-param",
						Title: "text-param",
						Type:  databricks.QueryParameterTypeNameText,
					},
					Value: "text-param",
				},
				&databricks.QueryParameterNumber{
					QueryParameter: databricks.QueryParameter{
						Name:  "number-param",
						Title: "number-param",
						Type:  databricks.QueryParameterTypeNameNumber,
					},
					Value: 1234.5,
				},
				&databricks.QueryParameterDateRange{
					QueryParameter: databricks.QueryParameter{
						Name:  "daterange-param",
						Title: "daterange-param",
						Type:  databricks.QueryParameterTypeNameDateRange,
					},
					Value: databricks.QueryParameterDateRangeValue{
						Start: "1234",
						End:   "9876",
					},
				},
				&databricks.QueryParameterDateRangeRelative{
					QueryParameter: databricks.QueryParameter{
						Name:  "daterangerelative-param",
						Title: "daterangerelative-param",
						Type:  databricks.QueryParameterTypeNameDateRange,
					},
					Value: "d_last_30_days",
				},
				&databricks.QueryParameterQuery{
					QueryParameter: databricks.QueryParameter{
						Name:  "query-param",
						Title: "query-param",
						Type:  databricks.QueryParameterTypeNameQuery,
					},
					Values:  []string{"hello", "world"},
					QueryID: "query id",
				},
				&databricks.QueryParameterQuery{
					QueryParameter: databricks.QueryParameter{
						Name:  "query-param-multi",
						Title: "query-param-multi",
						Type:  databricks.QueryParameterTypeNameQuery,
					},
					Values:  []string{"hello", "world", "multi"},
					QueryID: "multi query id",
					Multi: &databricks.QueryParameterMultipleValuesOptions{
						Prefix:    "'",
						Suffix:    "'",
						Separator: ",",
					},
				},
			},
		},
	}

	flattened := flattenQuery(query)
	expanded := expandQuery(flattened)

	assert.Equal(t, expanded, query)
}
