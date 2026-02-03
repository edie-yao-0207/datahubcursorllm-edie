package databricks

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMarshalUnmarshalVisualization(t *testing.T) {
	vis := Visualization{
		ID:          "123",
		QueryID:     "query-id",
		Type:        "TABLE",
		Name:        "test name",
		Description: "test description",
	}

	marshalled, err := json.Marshal(vis)
	require.NoError(t, err)

	var v Visualization
	err = json.Unmarshal(marshalled, &v)
	require.NoError(t, err)

	assert.Equal(t, vis, v)
}
