package databricks

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"samsaradev.io/infra/dataplatform/databricks"
)

func TestFlattenExpendSqlDashboard(t *testing.T) {
	dash := databricks.SqlDashboard{
		Name: "test dash name",
		Tags: []string{"test", "tags"},
	}

	flattened := flattenSqlDashboard(&dash)
	expanded := expandSqlDashboard(flattened)
	assert.Equal(t, expanded, &dash)
}
