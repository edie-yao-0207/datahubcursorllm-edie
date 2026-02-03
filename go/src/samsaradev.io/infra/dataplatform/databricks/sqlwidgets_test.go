package databricks

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWidgetMarshalUnmarshal(t *testing.T) {
	visID := "vis-ID"
	visText := "text"

	w := Widget{
		ID:          "12345",
		DashboardID: "dashboardID",

		VisualizationID: &visID,
		Text:            &visText,

		Options: WidgetOptions{
			ParameterMapping: map[string]WidgetParameterMapping{
				"p1": {
					Name:  "p1",
					Type:  "text",
					MapTo: "mapTo",
					Title: "title",
				},
			},
			Position: &WidgetPosition{
				AutoHeight: false,
				SizeX:      1,
				SizeY:      2,
				PosX:       3,
				PosY:       4,
			},
		},
	}

	out, err := json.Marshal(w)
	require.NoError(t, err)

	var wp Widget
	err = json.Unmarshal(out, &wp)
	require.NoError(t, err)

	assert.Equal(t, w, wp)
}

func TestWidgetUnmarshalWithVisualization(t *testing.T) {
	w := Widget{
		Visualization: json.RawMessage(`
			{
				"id": "12345"
			}
		`),
	}

	out, err := json.Marshal(w)
	require.NoError(t, err)

	var wp Widget
	err = json.Unmarshal(out, &wp)
	require.NoError(t, err)

	if assert.NotNil(t, wp.VisualizationID) {
		assert.Equal(t, "12345", string(*wp.VisualizationID))
	}
}
