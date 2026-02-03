package s3tables

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadS3TableFiles(t *testing.T) {
	s3Tables, err := ReadS3TableFiles()
	require.NoError(t, err)
	assert.True(t, len(s3Tables) != 0)
}
