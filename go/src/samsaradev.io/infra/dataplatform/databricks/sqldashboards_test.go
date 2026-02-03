package databricks

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMarshalUnmarshalSqlDashboard(t *testing.T) {
	sqlDashboard := SqlDashboard{
		Id:   "123",
		Name: "test name",
		Tags: []string{"test", "tags"},
	}

	marshalled, err := json.Marshal(sqlDashboard)
	require.NoError(t, err)

	var d SqlDashboard
	err = json.Unmarshal(marshalled, &d)
	require.NoError(t, err)

	assert.Equal(t, sqlDashboard, d)
}
