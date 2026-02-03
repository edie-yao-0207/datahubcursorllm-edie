package databricks

import (
	"context"
	"net/http"

	"github.com/samsarahq/go/oops"
)

type QueryStatus string

const (
	QueryStatusQueued   = QueryStatus("QUEUED")
	QueryStatusRunning  = QueryStatus("RUNNING")
	QueryStatusCanceled = QueryStatus("CANCELED")
	QueryStatusFailed   = QueryStatus("FAILED")
	QueryStatusFinished = QueryStatus("FINISHED")
)

type TimeRange struct {
	StartTimeMs int64 `json:"start_time_ms"`
	EndTimeMs   int64 `json:"end_time_ms"`
}

type QueryFilter struct {
	Statuses            []QueryStatus `json:"statuses,omitempty"`
	UserIds             []int         `json:"user_ids,omitempty"`
	EndpointIds         []string      `json:"endpoint_ids,omitempty"`
	QueryStartTimeRange TimeRange     `json:"query_start_time_range,omitempty"`
}

type QueryInfo struct {
	QueryId            string      `json:"query_id"`
	Status             QueryStatus `json:"status"`
	QueryText          string      `json:"query_text"`
	QueryStartTimeMs   int64       `json:"query_start_time_ms"`
	ExecutionEndTimeMs int64       `json:"execution_end_time_ms"`
	QueryEndTimeMs     int64       `json:"query_end_time_ms"`
	UserId             int64       `json:"user_id"`
	UserName           string      `json:"user_name"`
	SparkUIUrl         string      `json:"spark_ui_url"`
	EndpointId         string      `json:"endpoint_id"`
	ErrorMessage       string      `json:"error_message"`
	RowsProduced       int         `json:"rows_produced"`
}

type ListSqlQueryHistoryInput struct {
	FilterBy   *QueryFilter `json:"filter_by,omitempty"`
	MaxResults int          `json:"max_results,omitempty"`
	PageToken  string       `json:"page_token,omitempty"`
}

type ListSqlQueryHistoryOutput struct {
	NextPageToken string       `json:"next_page_token"`
	HasNextPage   bool         `json:"has_next_page"`
	Results       []*QueryInfo `json:"res"`
}

// https://docs.databricks.com/sql/api/query-history.html
type SqlQueryHistoryAPI interface {
	ListSqlQueryHistory(context.Context, *ListSqlQueryHistoryInput) (*ListSqlQueryHistoryOutput, error)
}

func (c *Client) ListSqlQueryHistory(ctx context.Context, input *ListSqlQueryHistoryInput) (*ListSqlQueryHistoryOutput, error) {
	var output ListSqlQueryHistoryOutput
	if err := c.do(ctx, http.MethodGet, "/api/2.0/sql/history/queries", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}
