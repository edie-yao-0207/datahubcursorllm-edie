package databricks

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"

	"samsaradev.io/infra/dataplatform/databricks"
)

func TestFlattenExpandSqlVisualization(t *testing.T) {
	vis := databricks.Visualization{
		QueryID:     "query id",
		Type:        "TABLE",
		Name:        "test vis name",
		Description: "test vis description",
		Options:     json.RawMessage(`{"key": "value}`),
	}

	flattened := flattenVisualization(&vis)
	expanded := expandVisualization(flattened)
	assert.Equal(t, expanded, &vis)
}
