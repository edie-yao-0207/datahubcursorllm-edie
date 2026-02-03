package databricks

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSqlQueryMarshalUnmarshal(t *testing.T) {
	query := Query{
		ID:           "id",
		DataSourceID: "abc123",
		Name:         "name",
		Description:  "description",
		Query:        "SELECT 1+1 AS 2",
		Schedule: &QuerySchedule{
			Interval: 100,
		},
		Tags:      []string{"tag1", "tag2"},
		RunAsRole: "viewer",
		Options: &QueryOptions{
			Parameters: []interface{}{
				&QueryParameterText{
					QueryParameter: QueryParameter{
						Name:  "text-param",
						Title: "text-param",
					},
					Value: "text-param",
				},
			},
		},
	}

	marshalled, err := json.Marshal(query)
	require.NoError(t, err)

	var q Query
	err = json.Unmarshal(marshalled, &q)
	require.NoError(t, err)

	assert.Equal(t, query, q)
}

func TestParamMarshalUnmarshal(t *testing.T) {
	testCases := []struct {
		paramType QueryParameterTypeName
		param     interface{}
	}{
		{
			paramType: QueryParameterTypeNameText,
			param: &QueryParameterText{
				QueryParameter: QueryParameter{
					Name:  "test-text",
					Title: "test-text",
				},
				Value: "test value",
			},
		},
		{
			paramType: QueryParameterTypeNameNumber,
			param: &QueryParameterNumber{
				QueryParameter: QueryParameter{
					Name:  "number-param",
					Title: "number-param",
				},
				Value: 1234.5,
			},
		},
		{
			paramType: QueryParameterTypeNameEnum,
			param: &QueryParameterEnum{
				QueryParameter: QueryParameter{
					Name:  "enum-param2",
					Title: "enum-param2",
				},
				Values:  []string{"multiple1", "multiple2"},
				Options: "foo\nbar",
				Multi: &QueryParameterMultipleValuesOptions{
					Prefix:    "@@@",
					Suffix:    "###",
					Separator: "$$$",
				},
			},
		},
		{
			paramType: QueryParameterTypeNameQuery,
			param: &QueryParameterQuery{
				QueryParameter: QueryParameter{
					Name:  "query-param2",
					Title: "query-param2",
				},
				Values:  []string{"multiple1", "multiple2"},
				QueryID: "queryID",
				Multi: &QueryParameterMultipleValuesOptions{
					Prefix:    "@@@",
					Suffix:    "###",
					Separator: "$$$",
				},
			},
		},
		{
			paramType: QueryParameterTypeNameDate,
			param: &QueryParameterDate{
				QueryParameter: QueryParameter{
					Name:  "date-param",
					Title: "date-param",
				},
				Value: "xyz",
			},
		},
		{
			paramType: QueryParameterTypeNameDateTime,
			param: &QueryParameterDateTime{
				QueryParameter: QueryParameter{
					Name:  "date-time-param",
					Title: "date-time-param",
				},
				Value: "xyz",
			},
		},
		{
			paramType: QueryParameterTypeNameDateTimeSec,
			param: &QueryParameterDateTimeSec{
				QueryParameter: QueryParameter{
					Name:  "date-time-sec-param",
					Title: "date-time-sec-param",
				},
				Value: "xyz",
			},
		},
		{
			paramType: QueryParameterTypeNameDateRange,
			param: &QueryParameterDateRange{
				QueryParameter: QueryParameter{
					Name:  "date-range-param",
					Title: "date-range-param",
				},
				Value: QueryParameterDateRangeValue{
					Start: "abc",
					End:   "xyz",
				},
			},
		},
		{
			paramType: QueryParameterTypeNameDateRange,
			param: &QueryParameterDateRangeRelative{
				QueryParameter: QueryParameter{
					Name:  "date-range-param",
					Title: "date-range-param",
				},
				Value: "last_7_days",
			},
		},
		{
			paramType: QueryParameterTypeNameDateTimeRange,
			param: &QueryParameterDateTimeRange{
				QueryParameter: QueryParameter{
					Name:  "date-time-range-param",
					Title: "date-time-range-param",
				},
				Value: "xyz",
			},
		},
		{
			paramType: QueryParameterTypeNameDateTimeSecRange,
			param: &QueryParameterDateTimeSecRange{
				QueryParameter: QueryParameter{
					Name:  "date-time-sec-range-param",
					Title: "date-time-sec-range-param",
				},
				Value: "xyz",
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(string(testCase.paramType), func(t *testing.T) {
			query := Query{
				ID:           "123",
				DataSourceID: "abc",
				Name:         "name",
				Description:  "description",
				Query:        "SELECT 1 AS one",
				Options: &QueryOptions{
					Parameters: []interface{}{
						testCase.param,
					},
				},
			}

			marshaled, err := json.Marshal(query)
			assert.NoError(t, err)

			var q Query
			err = json.Unmarshal(marshaled, &q)
			assert.NoError(t, err)

			assert.Equal(t, query, q)
		})
	}
}
