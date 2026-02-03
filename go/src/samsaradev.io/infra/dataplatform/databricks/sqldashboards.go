package databricks

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/samsarahq/go/oops"
)

const dashboardAPIRoot = "/api/2.0/preview/sql/dashboards"

// Sql Dashbooard API docs: https://docs.databricks.com/sql/api/queries-dashboards.html#tag/Dashboards
type SqlDashboard struct {
	Id      string            `json:"id"`
	Name    string            `json:"name"`
	Tags    []string          `json:"tags,omitempty"`
	Widgets []json.RawMessage `json:"widgets,omitempty"`
}

type SqlDashboardsAPI interface {
	CreateSqlDashboard(context.Context, *SqlDashboard) (*SqlDashboard, error)
	GetSqlDashboard(context.Context, string) (*SqlDashboard, error)
	EditSqlDashboard(context.Context, string, *SqlDashboard) error
	DeleteSqlDashboard(context.Context, string) error
}

func (c *Client) CreateSqlDashboard(ctx context.Context, input *SqlDashboard) (*SqlDashboard, error) {
	var d SqlDashboard
	if err := c.do(ctx, http.MethodPost, dashboardAPIRoot, input, &d); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &d, nil
}

func (c *Client) GetSqlDashboard(ctx context.Context, dashboardID string) (*SqlDashboard, error) {
	// Trash arguments required by c.do() but we do not care about.
	type getSqlDashboardInput struct{}
	payload := &getSqlDashboardInput{}

	var output SqlDashboard
	if err := c.do(ctx, http.MethodGet, dashboardAPIRoot+"/"+dashboardID, payload, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) EditSqlDashboard(ctx context.Context, dashboardID string, dashboard *SqlDashboard) error {
	// Trash arguments required by c.do() but we do not care about.
	type editSqlDashboardOutput struct{}
	resp := &editSqlDashboardOutput{}

	if err := c.do(ctx, http.MethodPost, dashboardAPIRoot+"/"+dashboardID, dashboard, resp); err != nil {
		return oops.Wrapf(err, "")
	}

	return nil
}

func (c *Client) DeleteSqlDashboard(ctx context.Context, dashboardID string) error {
	// Trash arguments required by c.do() but we do not care about.
	type deleteSqlDashboardInput struct{}
	payload := &deleteSqlDashboardInput{}
	type deleteSqlDashboardOutput struct{}
	resp := &deleteSqlDashboardOutput{}

	if err := c.do(ctx, http.MethodDelete, dashboardAPIRoot+"/"+dashboardID, payload, resp); err != nil {
		return oops.Wrapf(err, "")
	}
	return nil
}
