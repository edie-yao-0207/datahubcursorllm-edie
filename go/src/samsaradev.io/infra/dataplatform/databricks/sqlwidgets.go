package databricks

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/samsarahq/go/oops"
)

const widgetsAPIRoot = "/api/2.0/preview/sql/widgets"

type Widget struct {
	ID          string `json:"id,omitempty"`
	DashboardID string `json:"dashboard_id"`

	// VisualizationID and Text are mutually exclusive. A widget either contains
	// text, or contains a visualization.
	VisualizationID *string `json:"visualization_id"`
	Text            *string `json:"text"`

	Options WidgetOptions `json:"options"`

	// This field is no longer in use, but is still required as part of the schema.
	// It's OK that the field value is 0 everywhere:
	// https://github.com/databrickslabs/terraform-provider-databricks/blob/c5461173e19d08dbf39178ae6f32bbf08abca383/sqlanalytics/api/widget.go#L22-L24
	Width int `json:"width"`

	//  Visualization is set only when retrieving an existing widget.
	Visualization json.RawMessage `json:"visualization,omitempty"`
}

type WidgetOptions struct {
	ParameterMapping map[string]WidgetParameterMapping `json:"parameterMappings"`
	Position         *WidgetPosition                   `json:"position,omitempty"`
	Title            string                            `json:"title,omitempty"`
	Description      string                            `json:"description,omitempty"`
}

type WidgetPosition struct {
	AutoHeight bool `json:"autoHeight"`
	SizeX      int  `json:"sizeX,omitempty"`
	SizeY      int  `json:"sizeY,omitempty"`
	PosX       int  `json:"col"`
	PosY       int  `json:"row"`
}

type WidgetParameterMapping struct {
	Name  string `json:"name"`
	Type  string `json:"type"`
	MapTo string `json:"mapTo,omitempty"`

	// The type of the value depends on the type of the parameter referred to by `name`.
	Value interface{} `json:"value"`

	// This title overrides the title given to this parameter by the query, if specified.
	Title string `json:"title,omitempty"`
}

func (w *Widget) UnmarshalJSON(b []byte) error {
	type localWidget Widget
	err := json.Unmarshal(b, (*localWidget)(w))
	if err != nil {
		return err
	}

	// If a visualization is configured, set the corresponding visualization ID.
	// The visualization ID is only used when creating or updating a widget
	// and not set when retrieving an existing widget with visualization.
	if w.Visualization != nil {
		var v Visualization
		err = json.Unmarshal(w.Visualization, &v)
		if err != nil {
			return err
		}
		w.VisualizationID = &v.ID
	}

	return nil
}

type SqlWidgetsAPI interface {
	CreateSQLWidget(context.Context, *Widget) (*Widget, error)
	GetSQLWidget(context.Context, string, string) (*Widget, error)
	EditSQLWidget(context.Context, string, *Widget) error
	DeleteSQLWidget(context.Context, string) error
}

func (c *Client) CreateSQLWidget(ctx context.Context, widget *Widget) (*Widget, error) {
	var w Widget
	if err := c.do(ctx, http.MethodPost, widgetsAPIRoot, widget, &w); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &w, nil
}

func (c *Client) GetSQLWidget(ctx context.Context, dashboardID, widgetID string) (*Widget, error) {
	dashboard, err := c.GetSqlDashboard(ctx, dashboardID)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	for _, w := range dashboard.Widgets {
		var widget Widget
		if err := json.Unmarshal(w, &widget); err != nil {
			return nil, oops.Wrapf(err, "")
		}

		if widget.ID == widgetID {
			widget.DashboardID = dashboardID
			return &widget, nil
		}
	}

	// Return not found.
	return nil, oops.Wrapf(HTTPStatusError{
		StatusCode: http.StatusNotFound,
		Status:     "NOT_FOUND",
		Body:       fmt.Sprintf("Cannot find widget %s attached to dashboard %s", widgetID, dashboardID),
	}, "")
}

func (c *Client) EditSQLWidget(ctx context.Context, widgetID string, widget *Widget) error {
	// Trash arguments required by c.do() but we do not care about.
	type editSQLWidgetOutput struct{}
	resp := &editSQLWidgetOutput{}

	if err := c.do(ctx, http.MethodPost, widgetsAPIRoot+"/"+widgetID, widget, resp); err != nil {
		return oops.Wrapf(err, "")
	}
	return nil
}

func (c *Client) DeleteSQLWidget(ctx context.Context, widgetID string) error {
	// Trash arguments required by c.do() but we do not care about.
	type deleteSqlWidgetInput struct{}
	payload := &deleteSqlWidgetInput{}
	type deleteSqlWidgetOutput struct{}
	resp := &deleteSqlWidgetOutput{}

	if err := c.do(ctx, http.MethodDelete, widgetsAPIRoot+"/"+widgetID, payload, resp); err != nil {
		return oops.Wrapf(err, "")
	}
	return nil
}
