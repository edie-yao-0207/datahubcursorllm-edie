package databricks

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/samsarahq/go/oops"
)

const queryAPIRoot = "/api/2.0/preview/sql/queries"

// Query API docs: https://docs.databricks.com/sql/api/queries-dashboards.html#tag/Queries-Results
type Query struct {
	ID             string            `json:"id,omitempty"`
	DataSourceID   string            `json:"data_source_id"`
	Name           string            `json:"name"`
	Description    string            `json:"description"`
	Query          string            `json:"query"`
	Schedule       *QuerySchedule    `json:"schedule"`
	RunAsRole      string            `json:"run_as_role,omitempty"`
	Options        *QueryOptions     `json:"options,omitempty"`
	Tags           []string          `json:"tags,omitempty"`
	Visualizations []json.RawMessage `json:"visualizations,omitempty"`
	Parent         string            `json:"parent,omitempty"`
}

type QuerySchedule struct {
	// Interval in seconds.
	//
	// For daily schedules, this MUST be a multiple of 86400.
	// For weekly schedules, this MUST be a multiple of 604800.
	Interval int `json:"interval"`

	// Time of day, for daily and weekly schedules.
	Time *string `json:"time"`

	// Day of week, for weekly schedules.
	DayOfWeek *string `json:"day_of_week"`

	// Schedule should be active until this date.
	Until *string `json:"until"`
}

type QueryOptions struct {
	Parameters    []interface{}     `json:"-"`
	RawParameters []json.RawMessage `json:"parameters,omitempty"`
}

func (o *QueryOptions) MarshalJSON() ([]byte, error) {
	if o.Parameters != nil {
		o.RawParameters = []json.RawMessage{}
		for _, p := range o.Parameters {
			b, err := json.Marshal(p)
			if err != nil {
				return nil, oops.Wrapf(err, "")
			}
			o.RawParameters = append(o.RawParameters, b)
		}
	}

	type localQueryOptions QueryOptions
	return json.Marshal((*localQueryOptions)(o))
}

type QueryParameter struct {
	Name  string                 `json:"name"`
	Title string                 `json:"title,omitempty"`
	Type  QueryParameterTypeName `json:"type"`
}

type QueryParameterTypeName string

const (
	QueryParameterTypeNameText             QueryParameterTypeName = "text"
	QueryParameterTypeNameNumber           QueryParameterTypeName = "number"
	QueryParameterTypeNameEnum             QueryParameterTypeName = "enum"
	QueryParameterTypeNameQuery            QueryParameterTypeName = "query"
	QueryParameterTypeNameDate             QueryParameterTypeName = "date"
	QueryParameterTypeNameDateTime         QueryParameterTypeName = "datetime-local"
	QueryParameterTypeNameDateTimeSec      QueryParameterTypeName = "datetime-with-seconds"
	QueryParameterTypeNameDateRange        QueryParameterTypeName = "date-range"
	QueryParameterTypeNameDateTimeRange    QueryParameterTypeName = "datetime-range"
	QueryParameterTypeNameDateTimeSecRange QueryParameterTypeName = "datetime-range-with-seconds"
)

type QueryParameterText struct {
	QueryParameter
	Value string `json:"value"`
}

func (p QueryParameterText) MarshalJSON() ([]byte, error) {
	p.QueryParameter.Type = QueryParameterTypeNameText
	type localQueryParameter QueryParameterText
	return json.Marshal((localQueryParameter)(p))
}

func (p *QueryParameterText) UnmarshalJSON(b []byte) error {
	type localQueryParameter QueryParameterText
	if err := json.Unmarshal(b, (*localQueryParameter)(p)); err != nil {
		return oops.Wrapf(err, "")
	}
	p.Type = ""
	return nil
}

type QueryParameterNumber struct {
	QueryParameter
	Value float64 `json:"value"`
}

func (p QueryParameterNumber) MarshalJSON() ([]byte, error) {
	p.QueryParameter.Type = QueryParameterTypeNameNumber
	type localQueryParameter QueryParameterNumber
	return json.Marshal((localQueryParameter)(p))
}

func (p *QueryParameterNumber) UnmarshalJSON(b []byte) error {
	type localQueryParameter QueryParameterNumber
	if err := json.Unmarshal(b, (*localQueryParameter)(p)); err != nil {
		return oops.Wrapf(err, "")
	}
	p.Type = ""
	return nil
}

type QueryParameterMultipleValuesOptions struct {
	Prefix    string `json:"prefix"`
	Suffix    string `json:"suffix"`
	Separator string `json:"separator"`
}

type QueryParameterEnum struct {
	QueryParameter
	Values  []string                             `json:"-"`
	Value   json.RawMessage                      `json:"value"`
	Options string                               `json:"enumOptions"`
	Multi   *QueryParameterMultipleValuesOptions `json:"multiValuesOptions,omitempty"`
}

func (p QueryParameterEnum) MarshalJSON() ([]byte, error) {
	p.QueryParameter.Type = QueryParameterTypeNameEnum

	// Set `Value` depending on multiple options being allowed or not.
	var err error
	if p.Multi == nil {
		// Set as single string.
		p.Value, err = json.Marshal(p.Values[0])
		if err != nil {
			return nil, oops.Wrapf(err, "")
		}
	} else {
		// Set as array of strings.
		p.Value, err = json.Marshal(p.Values)
		if err != nil {
			return nil, oops.Wrapf(err, "")
		}
	}

	type localQueryParameter QueryParameterEnum
	return json.Marshal((localQueryParameter)(p))
}

// UnmarshalJSON deals with polymorphism of the `value` field.
func (p *QueryParameterEnum) UnmarshalJSON(b []byte) error {
	type localQueryParameter QueryParameterEnum
	err := json.Unmarshal(b, (*localQueryParameter)(p))
	if err != nil {
		return oops.Wrapf(err, "")
	}

	// If multiple options aren't configured, assume `value` is a string.
	// Otherwise, it's an array of strings.
	if p.Multi == nil {
		var v string
		err = json.Unmarshal(p.Value, &v)
		if err != nil {
			return nil
		}
		p.Values = []string{v}
	} else {
		var vs []string
		err = json.Unmarshal(p.Value, &vs)
		if err != nil {
			return nil
		}
		p.Values = vs
	}

	p.Type = ""
	p.Value = nil
	return nil
}

type QueryParameterQuery struct {
	QueryParameter
	Values  []string                             `json:"-"`
	Value   json.RawMessage                      `json:"value"`
	QueryID string                               `json:"queryId"`
	Multi   *QueryParameterMultipleValuesOptions `json:"multiValuesOptions,omitempty"`
}

func (p QueryParameterQuery) MarshalJSON() ([]byte, error) {
	p.QueryParameter.Type = QueryParameterTypeNameQuery

	// Set `Value` depending on multiple options being allowed or not.
	var err error
	if p.Multi == nil {
		// Set as single string.
		p.Value, err = json.Marshal(p.Values[0])
		if err != nil {
			return nil, oops.Wrapf(err, "")
		}
	} else {
		// Set as array of strings.
		p.Value, err = json.Marshal(p.Values)
		if err != nil {
			return nil, oops.Wrapf(err, "")
		}
	}

	type localQueryParameter QueryParameterQuery
	return json.Marshal((localQueryParameter)(p))
}

// UnmarshalJSON deals with polymorphism of the `value` field.
func (p *QueryParameterQuery) UnmarshalJSON(b []byte) error {
	type localQueryParameter QueryParameterQuery
	err := json.Unmarshal(b, (*localQueryParameter)(p))
	if err != nil {
		return oops.Wrapf(err, "")
	}

	// If multiple options aren't configured, assume `value` is a string.
	// Otherwise, it's an array of strings.
	if p.Multi == nil {
		var v string
		err = json.Unmarshal(p.Value, &v)
		if err != nil {
			return nil
		}
		p.Values = []string{v}
	} else {
		var vs []string
		err = json.Unmarshal(p.Value, &vs)
		if err != nil {
			return nil
		}
		p.Values = vs
	}

	p.Type = ""
	p.Value = nil
	return nil
}

type QueryParameterDate struct {
	QueryParameter
	Value string `json:"value"`
}

func (p QueryParameterDate) MarshalJSON() ([]byte, error) {
	p.QueryParameter.Type = QueryParameterTypeNameDate
	type localQueryParameter QueryParameterDate
	return json.Marshal((localQueryParameter)(p))
}

func (p *QueryParameterDate) UnmarshalJSON(b []byte) error {
	type localQueryParameter QueryParameterDate
	if err := json.Unmarshal(b, (*localQueryParameter)(p)); err != nil {
		return oops.Wrapf(err, "")
	}
	p.Type = ""
	return nil
}

type QueryParameterDateTime struct {
	QueryParameter
	Value string `json:"value"`
}

func (p QueryParameterDateTime) MarshalJSON() ([]byte, error) {
	p.QueryParameter.Type = QueryParameterTypeNameDateTime
	type localQueryParameter QueryParameterDateTime
	return json.Marshal((localQueryParameter)(p))
}

func (p *QueryParameterDateTime) UnmarshalJSON(b []byte) error {
	type localQueryParameter QueryParameterDateTime
	if err := json.Unmarshal(b, (*localQueryParameter)(p)); err != nil {
		return oops.Wrapf(err, "")
	}
	p.Type = ""
	return nil
}

type QueryParameterDateTimeSec struct {
	QueryParameter
	Value string `json:"value"`
}

func (p QueryParameterDateTimeSec) MarshalJSON() ([]byte, error) {
	p.QueryParameter.Type = QueryParameterTypeNameDateTimeSec
	type localQueryParameter QueryParameterDateTimeSec
	return json.Marshal((localQueryParameter)(p))
}

func (p *QueryParameterDateTimeSec) UnmarshalJSON(b []byte) error {
	type localQueryParameter QueryParameterDateTimeSec
	if err := json.Unmarshal(b, (*localQueryParameter)(p)); err != nil {
		return oops.Wrapf(err, "")
	}
	p.Type = ""
	return nil
}

type QueryParameterDateRange struct {
	QueryParameter
	Value QueryParameterDateRangeValue `json:"value"`
}

type QueryParameterDateRangeValue struct {
	Start string `json:"start"`
	End   string `json:"end"`
}

func (p QueryParameterDateRange) MarshalJSON() ([]byte, error) {
	p.QueryParameter.Type = QueryParameterTypeNameDateRange
	type localQueryParameter QueryParameterDateRange
	return json.Marshal((localQueryParameter)(p))
}

func (p *QueryParameterDateRange) UnmarshalJSON(b []byte) error {
	type localQueryParameter QueryParameterDateRange
	if err := json.Unmarshal(b, (*localQueryParameter)(p)); err != nil {
		return oops.Wrapf(err, "")
	}
	p.Type = ""
	return nil
}

type QueryParameterDateRangeRelative struct {
	QueryParameter
	Value string `json:"value"`
}

func (p QueryParameterDateRangeRelative) MarshalJSON() ([]byte, error) {
	p.QueryParameter.Type = QueryParameterTypeNameDateRange
	type localQueryParameter QueryParameterDateRangeRelative
	return json.Marshal((localQueryParameter)(p))
}

func (p *QueryParameterDateRangeRelative) UnmarshalJSON(b []byte) error {
	type localQueryParameter QueryParameterDateRangeRelative
	if err := json.Unmarshal(b, (*localQueryParameter)(p)); err != nil {
		return oops.Wrapf(err, "")
	}
	p.Type = ""
	return nil
}

type QueryParameterDateTimeRange struct {
	QueryParameter
	Value string `json:"value"`
}

func (p QueryParameterDateTimeRange) MarshalJSON() ([]byte, error) {
	p.QueryParameter.Type = QueryParameterTypeNameDateTimeRange
	type localQueryParameter QueryParameterDateTimeRange
	return json.Marshal((localQueryParameter)(p))
}

func (p *QueryParameterDateTimeRange) UnmarshalJSON(b []byte) error {
	type localQueryParameter QueryParameterDateTimeRange
	if err := json.Unmarshal(b, (*localQueryParameter)(p)); err != nil {
		return oops.Wrapf(err, "")
	}
	p.Type = ""
	return nil
}

type QueryParameterDateTimeSecRange struct {
	QueryParameter
	Value string `json:"value"`
}

func (p QueryParameterDateTimeSecRange) MarshalJSON() ([]byte, error) {
	p.QueryParameter.Type = QueryParameterTypeNameDateTimeSecRange
	type localQueryParameter QueryParameterDateTimeSecRange
	return json.Marshal((localQueryParameter)(p))
}

func (p *QueryParameterDateTimeSecRange) UnmarshalJSON(b []byte) error {
	type localQueryParameter QueryParameterDateTimeSecRange
	if err := json.Unmarshal(b, (*localQueryParameter)(p)); err != nil {
		return oops.Wrapf(err, "")
	}
	p.Type = ""
	return nil
}

func (o *QueryOptions) UnmarshalJSON(b []byte) error {
	type localQueryOptions QueryOptions
	err := json.Unmarshal(b, (*localQueryOptions)(o))
	if err != nil {
		return oops.Wrapf(err, "")
	}

	o.Parameters = []interface{}{}
	for _, rp := range o.RawParameters {
		var qp QueryParameter

		// Unmarshal into base parameter type to figure out the right type.
		err = json.Unmarshal(rp, &qp)
		if err != nil {
			return oops.Wrapf(err, "")
		}

		// Acquire pointer to the correct parameter type.
		var i interface{}
		switch qp.Type {
		case QueryParameterTypeNameText:
			i = &QueryParameterText{}
		case QueryParameterTypeNameNumber:
			i = &QueryParameterNumber{}
		case QueryParameterTypeNameEnum:
			i = &QueryParameterEnum{}
		case QueryParameterTypeNameQuery:
			i = &QueryParameterQuery{}
		case QueryParameterTypeNameDate:
			i = &QueryParameterDate{}
		case QueryParameterTypeNameDateTime:
			i = &QueryParameterDateTime{}
		case QueryParameterTypeNameDateTimeSec:
			i = &QueryParameterDateTimeSec{}
		case QueryParameterTypeNameDateRange:
			// Determine if this is a relative or absolute date range.
			// Default to Relative param.
			i = &QueryParameterDateRangeRelative{}

			// If we can successfully unmarshal into an Absolute param, then
			// this is an absolute param.
			var dra QueryParameterDateRange
			if err := json.Unmarshal(rp, &dra); err == nil {
				i = &QueryParameterDateRange{}
			}
		case QueryParameterTypeNameDateTimeRange:
			i = &QueryParameterDateTimeRange{}
		case QueryParameterTypeNameDateTimeSecRange:
			i = &QueryParameterDateTimeSecRange{}
		default:
			panic("don't know what to do...")
		}

		// Unmarshal into correct parameter type.
		err = json.Unmarshal(rp, &i)
		if err != nil {
			return oops.Wrapf(err, "")
		}

		o.Parameters = append(o.Parameters, i)
	}

	return nil
}

type SqlQueryAPI interface {
	CreateSqlQuery(context.Context, *Query) (*Query, error)
	GetSqlQuery(context.Context, string) (*Query, error)
	EditSqlQuery(context.Context, string, *Query) error
	DeleteSqlQuery(context.Context, string) error
}

func (c *Client) CreateSqlQuery(ctx context.Context, input *Query) (*Query, error) {
	var q Query
	if err := c.do(ctx, http.MethodPost, queryAPIRoot, input, &q); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &q, nil
}

func (c *Client) GetSqlQuery(ctx context.Context, queryID string) (*Query, error) {
	// Trash arguments required by c.do() but we do not care about.
	type getSqlQueryInput struct{}
	payload := &getSqlQueryInput{}

	var output Query
	if err := c.do(ctx, http.MethodGet, queryAPIRoot+"/"+queryID, payload, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) EditSqlQuery(ctx context.Context, queryID string, query *Query) error {
	// Trash arguments required by c.do() but we do not care about.
	type editQueryOutput struct{}
	resp := &editQueryOutput{}

	if err := c.do(ctx, http.MethodPost, queryAPIRoot+"/"+queryID, query, resp); err != nil {
		return oops.Wrapf(err, "")
	}
	return nil
}

func (c *Client) DeleteSqlQuery(ctx context.Context, queryID string) error {
	// Trash arguments required by c.do() but we do not care about.
	type deleteQueryInput struct{}
	payload := &deleteQueryInput{}
	type deleteQueryOutput struct{}
	resp := &deleteQueryOutput{}

	if err := c.do(ctx, http.MethodDelete, queryAPIRoot+"/"+queryID, payload, resp); err != nil {
		return oops.Wrapf(err, "")
	}
	return nil
}
