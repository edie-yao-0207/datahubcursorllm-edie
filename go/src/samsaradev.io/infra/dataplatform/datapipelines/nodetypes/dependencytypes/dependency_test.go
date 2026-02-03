package dependencytypes

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseRawDependencies(t *testing.T) {
	dep := &TableDependency{
		BaseDependency: BaseDependency{
			Type: TableDependencyType,
		},
		DBName:    "db1",
		TableName: "t1",
	}

	rawDep, err := json.Marshal(dep)
	require.NoError(t, err)
	parsedDep, err := ParseRawDependency(rawDep)
	require.NoError(t, err)
	require.Equal(t, dep, parsedDep)
}
