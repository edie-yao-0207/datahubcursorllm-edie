package s3views

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadSQLViewFiles(t *testing.T) {
	sqlViews, err := ReadSQLViewFiles()
	require.NoError(t, err)
	assert.True(t, len(sqlViews) != 0)
}
