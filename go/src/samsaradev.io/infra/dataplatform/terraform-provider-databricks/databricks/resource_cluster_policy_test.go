package databricks

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"samsaradev.io/infra/dataplatform/databricks"
)

func TestExpandFlattenClusterPolicy(t *testing.T) {
	cp := databricks.ClusterPolicy{
		PolicyId:             "policy_id",
		Name:                 "name",
		Definition:           "def",
		CreatorUserName:      "someone",
		CreatedAtTimestampMs: 100000,
	}

	flattened, err := flattenClusterPolicy(&cp)
	require.NoError(t, err)
	expanded, err := expandClusterPolicy(flattened)
	require.NoError(t, err)
	require.NotNil(t, expanded)
	assert.Equal(t, cp, *expanded)
}
