package govramp

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"samsaradev.io/infra/dataplatform/govramp"
)

func TestGetSchemaNamesAndBuildResources(t *testing.T) {
	tables := []govramp.Table{
		// Non-sharded schema
		{
			SourceCatalog: "default",
			SourceSchema:  "clouddb",
			SourceTable:   "groups",
			TableType:     govramp.SourceTableTypeTable,
		},
		// Sharded logical schema
		{
			SourceCatalog: "default",
			SourceSchema:  "workforcevideodb_shards",
			SourceTable:   "areas_of_interest",
			TableType:     govramp.SourceTableTypeView,
			TableCategory: govramp.SourceTableCategoryRdsSharded,
		},
		// Duplicate schema should be ignored
		{
			SourceCatalog: "default",
			SourceSchema:  "clouddb",
			SourceTable:   "addresses",
			TableType:     govramp.SourceTableTypeTable,
		},
		{
			SourceCatalog: "default",
			SourceSchema:  "kinesisstats",
			SourceTable:   "osdcm3xthermalsensors",
			TableType:     govramp.SourceTableTypeView,
			TableCategory: govramp.SourceTableCategoryKinesisstats,
		},
	}

	names, err := getSchemaNamesFromRegistry(tables)
	assert.NoError(t, err)

	// Expect base schemas: clouddb, workforcevideodb_shards
	// And shard schemas: workforcevideo_shard_0db, workforcevideo_shard_1db, ...
	expectedSchemas := []string{
		"clouddb",
		"kinesisstats",
		"workforcevideo_shard_0db",
		"workforcevideo_shard_1db",
		"workforcevideo_shard_2db",
		"workforcevideo_shard_3db",
		"workforcevideo_shard_4db",
		"workforcevideo_shard_5db",
		"workforcevideodb_shards",
	}
	// Names slice should be sorted and equal to expected order
	assert.Equal(t, expectedSchemas, names)
}
