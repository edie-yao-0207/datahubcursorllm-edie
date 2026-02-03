package databricks

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/samsarahq/go/oops"
)

const visualizationAPIRoot = "/api/2.0/preview/sql/visualizations"

type Visualization struct {
	ID          string          `json:"id,omitempty"`
	QueryID     string          `json:"query_id,omitempty"`
	Type        string          `json:"type"`
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Options     json.RawMessage `json:"options,omitempty"`
}

type SqlVisualizationsAPI interface {
	CreateSQLVisualization(context.Context, *Visualization) (*Visualization, error)
	GetSQLVisualization(context.Context, string, string) (*Visualization, error)
	EditSQLVisualization(context.Context, string, *Visualization) error
	DeleteSQLVisualization(context.Context, string) error
}

func (c *Client) CreateSQLVisualization(ctx context.Context, input *Visualization) (*Visualization, error) {
	var v Visualization
	if err := c.do(ctx, http.MethodPost, visualizationAPIRoot, input, &v); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &v, nil
}

func (c *Client) GetSQLVisualization(ctx context.Context, queryID, visualiationID string) (*Visualization, error) {
	query, err := c.GetSqlQuery(ctx, queryID)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	// Look for the visualization in the query.
	for _, v := range query.Visualizations {
		var vis Visualization
		if err := json.Unmarshal(v, &vis); err != nil {
			return nil, oops.Wrapf(err, "")
		}

		if vis.ID == visualiationID {
			vis.QueryID = queryID
			return &vis, nil
		}
	}

	// Return not found.
	return nil, oops.Wrapf(HTTPStatusError{
		StatusCode: http.StatusNotFound,
		Status:     "NOT_FOUND",
		Body:       fmt.Sprintf("Cannot find visualization %s attached to query %s", visualiationID, queryID),
	}, "")
}

func (c *Client) EditSQLVisualization(ctx context.Context, visualiationID string, visualization *Visualization) error {
	// Trash arguments required by c.do() but we do not care about.
	type editSQLVisualizationOutput struct{}
	resp := &editSQLVisualizationOutput{}

	if err := c.do(ctx, http.MethodPost, visualizationAPIRoot+"/"+visualiationID, visualization, resp); err != nil {
		return oops.Wrapf(err, "")
	}
	return nil
}

func (c *Client) DeleteSQLVisualization(ctx context.Context, visualiationID string) error {
	// Trash arguments required by c.do() but we do not care about.
	type deleteSqlVisualizationInput struct{}
	payload := &deleteSqlVisualizationInput{}
	type deleteSqlVisualizationOutput struct{}
	resp := &deleteSqlVisualizationOutput{}

	if err := c.do(ctx, http.MethodDelete, visualizationAPIRoot+"/"+visualiationID, payload, resp); err != nil {
		return oops.Wrapf(err, "")
	}
	return nil
}
