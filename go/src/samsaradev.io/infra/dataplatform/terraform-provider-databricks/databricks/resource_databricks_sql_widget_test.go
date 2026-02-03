package databricks

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"samsaradev.io/infra/dataplatform/databricks"
)

func TestFlattenExpandWidget(t *testing.T) {
	visID := "vis-ID"
	visText := "text"

	testCases := []struct {
		desc   string
		widget *databricks.Widget
	}{
		{
			desc: "widget with no value",
			widget: &databricks.Widget{
				DashboardID: "dashboardID",

				VisualizationID: &visID,
				Text:            &visText,

				Options: databricks.WidgetOptions{
					Title:       "option title",
					Description: "description title",
					ParameterMapping: map[string]databricks.WidgetParameterMapping{
						"p1": {
							Name:  "p1",
							Type:  "text",
							MapTo: "mapTo",
							Title: "title",
						},
					},
					Position: &databricks.WidgetPosition{
						AutoHeight: false,
						SizeX:      1,
						SizeY:      2,
						PosX:       3,
						PosY:       4,
					},
				},
			},
		},
		{
			desc: "widget with both value types",
			widget: &databricks.Widget{
				DashboardID: "dashboardID",

				VisualizationID: &visID,
				Text:            &visText,

				Options: databricks.WidgetOptions{
					Title:       "option title",
					Description: "description title",
					ParameterMapping: map[string]databricks.WidgetParameterMapping{
						"p1": {
							Name:  "p1",
							Type:  "text",
							MapTo: "mapTo",
							Title: "title",
							Value: "string value",
						},
						"p2": {
							Name:  "p2",
							Type:  "number",
							MapTo: "mapTo",
							Title: "title",
							Value: []string{"slice", "value"},
						},
					},
					Position: &databricks.WidgetPosition{
						AutoHeight: false,
						SizeX:      1,
						SizeY:      2,
						PosX:       3,
						PosY:       4,
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			flattened := flattenWidget(tc.widget)
			expanded := expandWidget(flattened)

			assert.Equal(t, expanded, tc.widget)
		})
	}
}
