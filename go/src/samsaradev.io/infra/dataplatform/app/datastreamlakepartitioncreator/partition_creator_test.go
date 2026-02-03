package main

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"samsaradev.io/infra/dataplatform/datastreamlake"
	"samsaradev.io/vendormocks/mock_glueiface"
)

func TestPartitionCreatorRun(t *testing.T) {
	mockGlueClient := mock_glueiface.NewMockGlueAPI(gomock.NewController(t))

	for i := 0; i < len(datastreamlake.Registry); i++ {
		// CreatePartition on datastream table
		mockGlueClient.EXPECT().CreatePartitionWithContext(gomock.Any(), gomock.Any())
	}

	partitionCreator, err := newPartitionCreator(mockGlueClient)
	require.Nil(t, err)
	assert.NoError(t, partitionCreator.CreatePartitions(context.Background(), "datastreams", "2022-01-01"))
}
